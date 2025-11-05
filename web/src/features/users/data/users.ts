// Static mock data for users
// In production, replace with actual API data

const statuses = ['active', 'inactive', 'invited', 'suspended'] as const
const roles = ['superadmin', 'admin', 'cashier', 'manager'] as const

function generateUserId(): string {
  return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`
}

function randomElement<T>(array: readonly T[]): T {
  return array[Math.floor(Math.random() * array.length)]
}

function randomDate(start: Date, end: Date): Date {
  return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()))
}

const sampleFirstNames = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Jessica', 'William', 'Ashley', 'James', 'Amanda', 'Christopher', 'Melissa', 'Daniel', 'Deborah']
const sampleLastNames = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Wilson', 'Anderson', 'Thomas', 'Taylor']

export const users = Array.from({ length: 500 }, () => {
  const firstName = randomElement(sampleFirstNames)
  const lastName = randomElement(sampleLastNames)
  const username = `${firstName.toLowerCase()}.${lastName.toLowerCase()}`
  const email = `${firstName.toLowerCase()}.${lastName.toLowerCase()}@example.com`
  
  return {
    id: generateUserId(),
    firstName,
    lastName,
    username,
    email,
    phoneNumber: `+1-${Math.floor(Math.random() * 900) + 100}-${Math.floor(Math.random() * 900) + 100}-${Math.floor(Math.random() * 9000) + 1000}`,
    status: randomElement(statuses),
    role: randomElement(roles),
    createdAt: randomDate(new Date(2020, 0, 1), new Date()),
    updatedAt: randomDate(new Date(Date.now() - 30 * 24 * 60 * 60 * 1000), new Date()),
  }
})
